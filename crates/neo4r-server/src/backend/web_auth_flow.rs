use super::*;

impl TcpBackend {
    pub(crate) fn request_database_name(&self, request: &HttpRequest) -> Result<String, String> {
        if let Some(query) = extract_optional_json_string_field(&request.body, "query")? {
            if let Some(database) = database_from_use_clause(&query)? {
                return Ok(database);
            }
        }
        let database = request
            .header("x-neo4r-database")
            .map(str::to_string)
            .or_else(|| request.query_value("db"))
            .or_else(|| {
                extract_optional_json_string_field(&request.body, "database")
                    .ok()
                    .flatten()
            })
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| DEFAULT_DATABASE.to_string());
        validate_database_name(&database)?;
        Ok(database)
    }

    pub(crate) fn database_for_name(&self, database: &str) -> Result<Neo4rDatabaseHandle, String> {
        validate_database_name(database)?;
        if database == DEFAULT_DATABASE {
            return Ok(self.db.clone());
        }
        self.tenant_databases
            .as_ref()
            .ok_or_else(|| "multi-tenant database manager is unavailable".to_string())?
            .database(database)
            .map_err(|err| err.to_string())
    }

    pub(crate) fn system_policy_json(&self) -> String {
        format!(
            "{{\"system_database\":\"system\",\"tenant_database_root\":\"databases\",\"system_metadata\":[\"web_auth\",\"web_audit\",\"web_sessions\"],\"tenant_backup_includes_system_metadata\":false,\"selected_database_header\":\"x-neo4r-database\"}}"
        )
    }

    pub(crate) fn authorized_role(&self, request: &HttpRequest, database: &str) -> Option<WebRole> {
        if let Some(session_id) =
            request_session_token(request).filter(|token| token.starts_with("sid:"))
        {
            if let Some(role) = self
                .web_sessions
                .as_ref()
                .and_then(|store| store.role_for_session(&session_id, database, unix_seconds_now()))
            {
                return Some(role);
            }
        }
        let Some(expected) = self.web_auth_token.as_ref() else {
            if self
                .web_user_tokens
                .as_ref()
                .and_then(|store| store.list().ok())
                .is_none_or(|users| users.is_empty())
            {
                return Some(WebRole::Admin);
            }
            let supplied = request
                .header("authorization")
                .and_then(|value| value.strip_prefix("Bearer "))
                .map(str::to_string)
                .or_else(|| request.query_value("token"))?;
            return self.web_user_tokens.as_ref().and_then(|store| {
                store.find_role_by_token(&supplied, database, unix_seconds_now())
            });
        };
        let supplied = request
            .header("authorization")
            .and_then(|value| value.strip_prefix("Bearer "))
            .map(str::to_string)
            .or_else(|| request.query_value("token"))?;
        if constant_time_token_eq(&supplied, expected) {
            return Some(web_role_from_token(expected));
        }
        self.web_user_tokens
            .as_ref()
            .and_then(|store| store.find_role_by_token(&supplied, database, unix_seconds_now()))
    }

    pub(crate) fn create_web_session(
        &self,
        request: &HttpRequest,
        database: &str,
    ) -> Result<String, String> {
        let token = extract_json_string_field(&request.body, "token").or_else(|_| {
            request
                .header("authorization")
                .and_then(|value| value.strip_prefix("Bearer "))
                .map(str::to_string)
                .ok_or_else(|| "missing token".to_string())
        })?;
        let role = self
            .authorize_token_value(&token, database)
            .ok_or_else(|| "unauthorized".to_string())?;
        let session_id = self
            .web_sessions
            .as_ref()
            .ok_or_else(|| "web session store is unavailable".to_string())?
            .create(&token, database, role, unix_seconds_now(), 3600)?;
        Ok(format!(
            "{{\"session_id\":\"{}\",\"csrf_token\":\"{}\",\"expires_in\":3600,\"database\":\"{}\"}}",
            json_escape(&session_id.session_id),
            json_escape(&session_id.csrf_token),
            json_escape(database)
        ))
    }

    pub(crate) fn delete_web_session(&self, request: &HttpRequest) -> Result<String, String> {
        let Some(session_id) =
            request_session_token(request).filter(|token| token.starts_with("sid:"))
        else {
            return Err("missing web session".to_string());
        };
        self.web_sessions
            .as_ref()
            .ok_or_else(|| "web session store is unavailable".to_string())?
            .delete(&session_id)?;
        Ok("{\"logged_out\":true}".to_string())
    }

    pub(crate) fn valid_session_csrf(&self, request: &HttpRequest) -> bool {
        let Some(session_id) =
            request_session_token(request).filter(|token| token.starts_with("sid:"))
        else {
            return false;
        };
        let Some(expected) = self
            .web_sessions
            .as_ref()
            .and_then(|store| store.csrf_for_session(&session_id, unix_seconds_now()))
        else {
            return false;
        };
        request
            .header("x-neo4r-csrf")
            .is_some_and(|supplied| constant_time_token_eq(supplied, &expected))
    }

    pub(crate) fn authorize_token_value(&self, token: &str, database: &str) -> Option<WebRole> {
        if let Some(expected) = self.web_auth_token.as_ref() {
            if constant_time_token_eq(token, expected) {
                return Some(web_role_from_token(expected));
            }
        }
        self.web_user_tokens
            .as_ref()
            .and_then(|store| store.find_role_by_token(token, database, unix_seconds_now()))
    }
}
