use super::*;

impl TcpBackend {
    pub(crate) fn deny_web_database(&self, body: &str) -> Result<String, String> {
        let name = extract_json_string_field(body, "name")?;
        let token_id = extract_json_string_field(body, "token_id")?;
        let database = extract_json_string_field(body, "database")?;
        let reason = extract_optional_json_string_field(body, "reason")?
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| "unspecified".to_string());
        validate_database_scope_for_admin(&database)?;
        self.web_user_tokens
            .as_ref()
            .ok_or_else(|| "web user token store is unavailable".to_string())?
            .deny_database(&name, &token_id, &database)?;
        self.audit_admin(
            "rbac.deny",
            &format!("{name}/{token_id}/{database}"),
            &format!("reason={reason}"),
        );
        self.web_users_json()
    }

    pub(crate) fn allow_web_database(&self, body: &str) -> Result<String, String> {
        let name = extract_json_string_field(body, "name")?;
        let token_id = extract_json_string_field(body, "token_id")?;
        let database = extract_json_string_field(body, "database")?;
        let reason = extract_optional_json_string_field(body, "reason")?
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| "unspecified".to_string());
        validate_database_scope_for_admin(&database)?;
        self.web_user_tokens
            .as_ref()
            .ok_or_else(|| "web user token store is unavailable".to_string())?
            .allow_database(&name, &token_id, &database)?;
        self.audit_admin(
            "rbac.allow",
            &format!("{name}/{token_id}/{database}"),
            &format!("reason={reason}"),
        );
        self.web_users_json()
    }
}

fn validate_database_scope_for_admin(database: &str) -> Result<(), String> {
    if database == "*" {
        Ok(())
    } else {
        validate_database_name(database)
    }
}
