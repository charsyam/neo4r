use super::CliError;

pub(super) fn normalize_cluster_subcommand(args: &[String]) -> Result<Vec<String>, CliError> {
    let command = match args.get(1).map(String::as_str).unwrap_or("status") {
        "status" => "CLUSTER_STATUS".to_string(),
        "topology" => "TOPOLOGY_OBSERVE".to_string(),
        "reconcile" => match args.get(2) {
            Some(limit) => format!("TOPOLOGY_RECONCILE\t{limit}"),
            None => "TOPOLOGY_RECONCILE".to_string(),
        },
        "chaos" => "CHAOS_CHECKS".to_string(),
        "promote" if args.len() >= 3 => format!("PROMOTE_CAUGHT_UP_NODE\t{}", args[2]),
        "bootstrap-manifest" if args.len() >= 5 => {
            format!(
                "WRITE_BOOTSTRAP_MANIFEST\t{}\t{}\t{}",
                args[2], args[3], args[4]
            )
        }
        "bootstrap-safety" if args.len() >= 4 => {
            format!("BOOTSTRAP_SAFETY\t{}\t{}", args[2], args[3])
        }
        "safety" if args.len() >= 3 => {
            let mut command = format!("OPERATIONAL_SAFETY\t{}", args[2]);
            if let Some(confirmation) = args.get(3) {
                command.push('\t');
                command.push_str(confirmation);
            }
            command
        }
        _ => {
            return Err(CliError::Usage(
                "cluster subcommand supports: status, topology, reconcile [LIMIT], chaos, promote SERVER_ID, bootstrap-manifest MODE CLUSTER_ID DB_ID, bootstrap-safety EXPECTED_CLUSTER_ID FORCE, safety OPERATION [TOKEN]".to_string(),
            ));
        }
    };
    Ok(vec!["--command".to_string(), command])
}
