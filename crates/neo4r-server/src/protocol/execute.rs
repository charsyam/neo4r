use super::format::*;
use super::*;

pub fn write_response(writer: &mut impl Write, response: &BackendResponse) -> io::Result<()> {
    writeln!(writer, "{}", format_response(response))
}

pub fn format_response(response: &BackendResponse) -> String {
    match response {
        BackendResponse::OkPong => "OK\tPONG".to_string(),
        BackendResponse::OkBye => "OK\tBYE".to_string(),
        BackendResponse::OkNode(id) => format!("OK\tNODE\t{id}"),
        BackendResponse::OkRelationship(id) => format!("OK\tRELATIONSHIP\t{id}"),
        BackendResponse::OkUnit => "OK".to_string(),
        BackendResponse::OkRows { count, debug_rows } => {
            format!("OK\tROWS\t{count}\t{}", escape_response(debug_rows))
        }
        BackendResponse::OkQueryPeers(peers) => {
            format!("OK\tQUERY_PEERS\t{}", escape_response(peers))
        }
        BackendResponse::OkReplicationPeers(peers) => {
            format!("OK\tREPLICATION_PEERS\t{}", escape_response(peers))
        }
        BackendResponse::OkGossip(gossip) => {
            format!("OK\tGOSSIP\t{}", escape_response(gossip))
        }
        BackendResponse::OkReplicationPeerStatus(status) => {
            format!("OK\tREPLICATION_PEER_STATUS\t{}", escape_response(status))
        }
        BackendResponse::OkReplicationStatus(status) => {
            format!("OK\tREPLICATION_STATUS\t{}", escape_response(status))
        }
        BackendResponse::OkRoutingTable(routing) => {
            format!("OK\tROUTING_TABLE\t{}", escape_response(routing))
        }
        BackendResponse::OkClusterRegistry(registry) => {
            format!("OK\tCLUSTER_REGISTRY\t{}", escape_response(registry))
        }
        BackendResponse::OkCapabilities(capabilities) => {
            format!("OK\tCAPABILITIES\t{}", escape_response(capabilities))
        }
        BackendResponse::OkCatchUp(results) => {
            format!("OK\tCATCH_UP\t{}", escape_response(results))
        }
        BackendResponse::OkCatchUpPlan(plan) => {
            format!("OK\tCATCH_UP_PLAN\t{}", escape_response(plan))
        }
        BackendResponse::OkTransactionDecisions(decisions) => {
            format!("OK\tTX_DECISIONS\t{}", escape_response(decisions))
        }
        BackendResponse::OkTransactionRecovery(count) => {
            format!("OK\tTX_RECOVERY\t{count}")
        }
        BackendResponse::OkClusterStatus(status) => {
            format!("OK\tCLUSTER_STATUS\t{}", escape_response(status))
        }
        BackendResponse::OkIndexCatalog(catalog) => {
            format!("OK\tINDEX_CATALOG\t{}", escape_response(catalog))
        }
        BackendResponse::OkVectorIndexStatus(status) => {
            format!("OK\tVECTOR_INDEX_STATUS\t{}", escape_response(status))
        }
        BackendResponse::OkQueryPlan(plan) => {
            format!("OK\tQUERY_PLAN\t{}", escape_response(plan))
        }
        BackendResponse::OkQueryProfile(profile) => {
            format!("OK\tPROFILE\t{}", escape_response(profile))
        }
        BackendResponse::OkStorageStatus(status) => {
            format!("OK\tSTORAGE_STATUS\t{}", escape_response(status))
        }
        BackendResponse::OkStatistics(statistics) => {
            format!("OK\tSTATISTICS\t{}", escape_response(statistics))
        }
        BackendResponse::OkStorageMaintenance(result) => {
            format!("OK\tSTORAGE_MAINTENANCE\t{}", escape_response(result))
        }
        BackendResponse::OkMetadataLog(log) => {
            format!("OK\tMETADATA_LOG\t{}", escape_response(log))
        }
        BackendResponse::OkClusterNodes(nodes) => {
            format!("OK\tCLUSTER_NODES\t{}", escape_response(nodes))
        }
        BackendResponse::OkRebalancePlan(plan) => {
            format!("OK\tREBALANCE_PLAN\t{}", escape_response(plan))
        }
        BackendResponse::OkRebalanceExecution(execution) => {
            format!("OK\tREBALANCE_EXECUTION\t{}", escape_response(execution))
        }
        BackendResponse::OkClusterManagementStatus(status) => {
            format!("OK\tCLUSTER_MANAGEMENT\t{}", escape_response(status))
        }
        BackendResponse::OkBootstrapManifest(manifest) => {
            format!("OK\tBOOTSTRAP_MANIFEST\t{}", escape_response(manifest))
        }
        BackendResponse::OkTopologyObservation(observation) => {
            format!("OK\tTOPOLOGY_OBSERVATION\t{}", escape_response(observation))
        }
        BackendResponse::OkOperationalSafety(safety) => {
            format!("OK\tOPERATIONAL_SAFETY\t{}", escape_response(safety))
        }
        BackendResponse::OkChaosChecks(checks) => {
            format!("OK\tCHAOS_CHECKS\t{}", escape_response(checks))
        }
        BackendResponse::Redirect(redirect) => format_redirect_response(redirect),
        BackendResponse::Err(message) => format!("ERR\t{}", escape_response(message)),
    }
}
