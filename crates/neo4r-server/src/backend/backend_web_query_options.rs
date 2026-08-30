use super::*;

pub(crate) fn parse_query_options(request: &HttpRequest) -> Result<QueryOptions, String> {
    let consistency = request
        .header("x-neo4r-read-consistency")
        .map(str::to_string)
        .or_else(|| {
            extract_optional_json_string_field(&request.body, "read_consistency")
                .ok()
                .flatten()
        })
        .unwrap_or_else(|| "strong".to_string());
    let normalized = consistency.trim().to_ascii_lowercase().replace('-', "_");
    let read_consistency = match normalized.as_str() {
        "strong" => ReadConsistency::Strong,
        "follower_stale" | "stale" => ReadConsistency::FollowerStale,
        "bounded_staleness" | "bounded" => {
            let max_staleness_ms = request
                .header("x-neo4r-max-staleness-ms")
                .and_then(|value| value.parse::<u64>().ok())
                .or_else(|| {
                    extract_optional_json_u64_field(&request.body, "max_staleness_ms")
                        .ok()
                        .flatten()
                })
                .unwrap_or(1_000);
            ReadConsistency::BoundedStaleness { max_staleness_ms }
        }
        other => {
            return Err(format!(
                "unsupported read_consistency {other}; expected strong, follower_stale, or bounded_staleness"
            ));
        }
    };
    Ok(QueryOptions::default().with_consistency(read_consistency))
}
