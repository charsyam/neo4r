use crate::result::QueryRow;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QuerySpillPolicy {
    pub operator_memory_budget_bytes: usize,
    pub spill_directory: PathBuf,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QuerySpillReport {
    pub spilled: bool,
    pub estimated_bytes: usize,
    pub manifest_path: Option<PathBuf>,
}

impl QuerySpillPolicy {
    pub fn new(operator_memory_budget_bytes: usize, spill_directory: impl Into<PathBuf>) -> Self {
        Self {
            operator_memory_budget_bytes,
            spill_directory: spill_directory.into(),
        }
    }

    pub fn from_env() -> Option<Self> {
        let budget = std::env::var("NEO4R_QUERY_OPERATOR_MEMORY_BUDGET_BYTES")
            .ok()?
            .parse::<usize>()
            .ok()?;
        let dir = std::env::var("NEO4R_QUERY_SPILL_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|_| std::env::temp_dir().join("neo4r-query-spill"));
        Some(Self::new(budget, dir))
    }
}

pub fn maybe_spill_operator_rows(
    operator_name: &str,
    rows: &[QueryRow],
    policy: Option<&QuerySpillPolicy>,
) -> std::io::Result<QuerySpillReport> {
    let estimated_bytes = estimate_rows_bytes(rows);
    let Some(policy) = policy else {
        return Ok(QuerySpillReport {
            spilled: false,
            estimated_bytes,
            manifest_path: None,
        });
    };
    if estimated_bytes <= policy.operator_memory_budget_bytes {
        return Ok(QuerySpillReport {
            spilled: false,
            estimated_bytes,
            manifest_path: None,
        });
    }
    fs::create_dir_all(&policy.spill_directory)?;
    let manifest_path = policy.spill_directory.join(format!(
        "{}-{}.spill",
        sanitize(operator_name),
        unix_millis_now()
    ));
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&manifest_path)?;
    writeln!(file, "query_spill_manifest:v1")?;
    writeln!(file, "operator={operator_name}")?;
    writeln!(file, "estimated_bytes={estimated_bytes}")?;
    writeln!(
        file,
        "operator_memory_budget_bytes={}",
        policy.operator_memory_budget_bytes
    )?;
    file.sync_all()?;
    Ok(QuerySpillReport {
        spilled: true,
        estimated_bytes,
        manifest_path: Some(manifest_path),
    })
}

pub fn cleanup_orphaned_spill_dirs(root: impl AsRef<Path>) -> std::io::Result<usize> {
    let root = root.as_ref();
    if !root.exists() {
        return Ok(0);
    }
    let mut removed = 0;
    for entry in fs::read_dir(root)? {
        let path = entry?.path();
        if path.extension().and_then(|value| value.to_str()) == Some("spill") {
            fs::remove_file(path)?;
            removed += 1;
        }
    }
    Ok(removed)
}

fn estimate_rows_bytes(rows: &[QueryRow]) -> usize {
    rows.iter()
        .map(|row| {
            row.values()
                .iter()
                .map(|(key, value)| key.len() + format!("{value:?}").len())
                .sum::<usize>()
        })
        .sum()
}

fn sanitize(value: &str) -> String {
    value
        .chars()
        .map(|ch| if ch.is_ascii_alphanumeric() { ch } else { '-' })
        .collect()
}

fn unix_millis_now() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::QueryValue;
    use neo4r_core::Value;

    #[test]
    fn spill_manifest_is_written_when_operator_budget_is_exceeded() {
        let dir =
            std::env::temp_dir().join(format!("neo4r-query-spill-test-{}", unix_millis_now()));
        let mut row = QueryRow::new();
        row.insert(
            "name".to_string(),
            QueryValue::Scalar(Value::String("alice".repeat(32))),
        );

        let report =
            maybe_spill_operator_rows("sort", &[row], Some(&QuerySpillPolicy::new(8, &dir)))
                .unwrap();

        assert!(report.spilled);
        let manifest = fs::read_to_string(report.manifest_path.unwrap()).unwrap();
        assert!(manifest.contains("query_spill_manifest:v1"));
        assert!(manifest.contains("operator=sort"));
        assert_eq!(cleanup_orphaned_spill_dirs(&dir).unwrap(), 1);
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn spill_is_skipped_when_rows_fit_budget() {
        let dir = std::env::temp_dir().join(format!("neo4r-query-spill-fit-{}", unix_millis_now()));
        let mut row = QueryRow::new();
        row.insert(
            "name".to_string(),
            QueryValue::Scalar(Value::String("a".to_string())),
        );

        let report = maybe_spill_operator_rows(
            "hash-aggregate",
            &[row],
            Some(&QuerySpillPolicy::new(1024, &dir)),
        )
        .unwrap();

        assert!(!report.spilled);
        assert!(report.manifest_path.is_none());
        assert!(!dir.exists());
    }
}
