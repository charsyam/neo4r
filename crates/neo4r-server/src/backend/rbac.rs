use super::{HttpRequest, WebRole};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum WebAction {
    GraphRead,
    GraphWrite,
    MetricsRead,
    SystemRead,
    TokenAdmin,
    TenantAdmin,
    AuditAdmin,
    BackupAdmin,
    RestoreAdmin,
    RaftAdmin,
    RepairAdmin,
    ClusterAdmin,
}

impl WebAction {
    pub(crate) fn required_role(self) -> WebRole {
        match self {
            Self::GraphRead | Self::MetricsRead | Self::SystemRead => WebRole::Reader,
            Self::GraphWrite => WebRole::Writer,
            Self::TokenAdmin
            | Self::TenantAdmin
            | Self::AuditAdmin
            | Self::BackupAdmin
            | Self::RestoreAdmin
            | Self::RaftAdmin
            | Self::RepairAdmin
            | Self::ClusterAdmin => WebRole::Admin,
        }
    }
}

pub(crate) fn web_role_allows_action(role: WebRole, action: WebAction) -> bool {
    role.allows(action.required_role())
}

pub(crate) fn web_action_for_request(request: &HttpRequest) -> Option<WebAction> {
    match (request.method.as_str(), request.path.as_str()) {
        ("GET", "/api/graph") | ("POST", "/api/query-plan") | ("POST", "/api/profile") => {
            Some(WebAction::GraphRead)
        }
        ("POST", "/api/query") => Some(WebAction::GraphWrite),
        ("GET", "/api/metrics") | ("GET", "/metrics") | ("GET", "/api/slow-queries") => {
            Some(WebAction::MetricsRead)
        }
        ("GET", "/")
        | ("GET", "/index.html")
        | ("GET", "/api/capabilities")
        | ("GET", "/api/admin/system-policy")
        | ("GET", "/api/admin/distributed-query")
        | ("GET", "/api/statistics")
        | ("GET", "/api/storage")
        | ("GET", "/api/metadata-log")
        | ("GET", "/api/cluster")
        | ("GET", "/api/cluster/routing-table")
        | ("GET", "/api/cluster/registry")
        | ("GET", "/api/database")
        | ("POST", "/api/use-database")
        | ("GET", "/api/examples") => Some(WebAction::SystemRead),
        ("GET", "/api/admin/users")
        | ("POST", "/api/admin/users")
        | ("POST", "/api/admin/users/delete")
        | ("POST", "/api/admin/users/tokens")
        | ("POST", "/api/admin/users/tokens/revoke")
        | ("POST", "/api/admin/grant-role")
        | ("POST", "/api/admin/revoke-role")
        | ("POST", "/api/admin/deny-role")
        | ("POST", "/api/admin/allow-role")
        | ("POST", "/api/admin/tokens/cleanup") => Some(WebAction::TokenAdmin),
        ("GET", "/api/admin/databases")
        | ("POST", "/api/admin/databases")
        | ("POST", "/api/admin/databases/drop")
        | ("POST", "/api/admin/databases/roles") => Some(WebAction::TenantAdmin),
        ("GET", "/api/admin/audit")
        | ("POST", "/api/admin/audit/prune")
        | ("GET", "/api/admin/audit/export") => Some(WebAction::AuditAdmin),
        ("POST", "/api/backup") => Some(WebAction::BackupAdmin),
        ("POST", "/api/restore")
        | ("POST", "/api/admin/restore-pitr")
        | ("POST", "/api/admin/restore-pitr/apply")
        | ("GET", "/api/admin/restore-pitr/pending")
        | ("POST", "/api/admin/restore-pitr/complete") => Some(WebAction::RestoreAdmin),
        ("POST", "/api/admin/raft/step-down")
        | ("POST", "/api/admin/raft/snapshot")
        | ("POST", "/api/admin/raft/transfer-leader") => Some(WebAction::RaftAdmin),
        ("GET", "/api/admin/repair") | ("POST", "/api/admin/repair") => {
            Some(WebAction::RepairAdmin)
        }
        ("POST", "/api/admin/cluster/migrate")
        | ("POST", "/api/admin/cluster/rebalance")
        | ("POST", "/api/admin/cluster/plan-node-add")
        | ("POST", "/api/admin/cluster/plan-node-remove") => Some(WebAction::ClusterAdmin),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rbac_policy_maps_actions_to_minimum_roles() {
        assert!(web_role_allows_action(
            WebRole::Reader,
            WebAction::GraphRead
        ));
        assert!(web_role_allows_action(
            WebRole::Reader,
            WebAction::MetricsRead
        ));
        assert!(!web_role_allows_action(
            WebRole::Reader,
            WebAction::GraphWrite
        ));
        assert!(web_role_allows_action(
            WebRole::Writer,
            WebAction::GraphWrite
        ));
        assert!(!web_role_allows_action(
            WebRole::Writer,
            WebAction::BackupAdmin
        ));
        assert!(!web_role_allows_action(
            WebRole::Writer,
            WebAction::RaftAdmin
        ));
        assert!(web_role_allows_action(
            WebRole::Admin,
            WebAction::TokenAdmin
        ));
        assert!(web_role_allows_action(
            WebRole::Admin,
            WebAction::RestoreAdmin
        ));
        assert!(web_role_allows_action(
            WebRole::Admin,
            WebAction::RepairAdmin
        ));
    }
}
