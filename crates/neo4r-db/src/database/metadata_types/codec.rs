use super::*;

pub(in crate::database) fn parse_plan_u64(input: &str, name: &str) -> DatabaseResult<u64> {
    input
        .parse::<u64>()
        .map_err(|_| StorageError::CorruptStore(format!("invalid {name}")).into())
}

pub(in crate::database) fn parse_plan_usize(input: &str, name: &str) -> DatabaseResult<usize> {
    input
        .parse::<usize>()
        .map_err(|_| StorageError::CorruptStore(format!("invalid {name}")).into())
}

pub(in crate::database) fn parse_plan_bool(input: &str, name: &str) -> DatabaseResult<bool> {
    match input {
        "0" => Ok(false),
        "1" => Ok(true),
        _ => Err(StorageError::CorruptStore(format!("invalid {name}")).into()),
    }
}

pub(in crate::database) fn sanitize_cluster_text(input: &str) -> String {
    input
        .chars()
        .map(|ch| {
            if matches!(ch, '\t' | '\n' | '\r') {
                ' '
            } else {
                ch
            }
        })
        .collect()
}
