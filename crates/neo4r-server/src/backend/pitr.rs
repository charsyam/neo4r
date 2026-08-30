use super::*;

impl TcpBackend {
    pub(crate) fn pitr_restore_plan_json(
        &self,
        db: &Neo4rDatabaseHandle,
        database_name: &str,
        body: &str,
    ) -> Result<String, String> {
        let target_physical_ms = extract_optional_json_u64_field(body, "target_physical_ms")?
            .ok_or_else(|| "target_physical_ms is required".to_string())?;
        let target_logical =
            extract_optional_json_u64_field(body, "target_logical")?.unwrap_or_default() as u32;
        let dry_run = extract_optional_json_bool_field(body, "dry_run")?;
        if !dry_run {
            return Err("PITR timestamp restore currently requires dry_run=true".to_string());
        }
        let shard_plans = self.pitr_restore_plan_shards(db, target_physical_ms, target_logical)?;
        self.audit_admin(
            "restore.pitr.plan",
            database_name,
            &format!("target_physical_ms={target_physical_ms} target_logical={target_logical}"),
        );
        Ok(format!(
            "{{\"database\":\"{}\",\"dry_run\":true,\"target_physical_ms\":{},\"target_logical\":{},\"shards\":[{}]}}",
            json_escape(database_name),
            target_physical_ms,
            target_logical,
            shard_plans.join(",")
        ))
    }

    pub(crate) fn pitr_restore_apply_json(
        &self,
        db: &Neo4rDatabaseHandle,
        database_name: &str,
        body: &str,
    ) -> Result<String, String> {
        let target_physical_ms = extract_optional_json_u64_field(body, "target_physical_ms")?
            .ok_or_else(|| "target_physical_ms is required".to_string())?;
        let target_logical =
            extract_optional_json_u64_field(body, "target_logical")?.unwrap_or_default() as u32;
        if extract_optional_json_string_field(body, "confirm")?.as_deref() != Some("RESTORE_PITR") {
            return Err("destructive PITR restore requires confirm=\"RESTORE_PITR\"".to_string());
        }
        let shard_plans = self.pitr_restore_plan_shards(db, target_physical_ms, target_logical)?;
        let data_dir = db.data_dir().map_err(|err| err.to_string())?;
        let manifest_path = data_dir.join("system").join("pitr-restore.pending");
        if let Some(parent) = manifest_path.parent() {
            std::fs::create_dir_all(parent).map_err(|err| err.to_string())?;
        }
        let manifest = format!(
            "pitr_restore_manifest:v1\ndatabase={}\ntarget_physical_ms={}\ntarget_logical={}\nshards={}\n",
            database_name,
            target_physical_ms,
            target_logical,
            shard_plans.join(";")
        );
        std::fs::write(&manifest_path, manifest).map_err(|err| err.to_string())?;
        self.audit_admin(
            "restore.pitr.apply",
            database_name,
            &format!(
                "target_physical_ms={target_physical_ms} target_logical={target_logical} manifest={}",
                manifest_path.display()
            ),
        );
        Ok(format!(
            "{{\"database\":\"{}\",\"accepted\":true,\"manifest\":\"{}\",\"target_physical_ms\":{},\"target_logical\":{},\"shards\":[{}]}}",
            json_escape(database_name),
            json_escape(&manifest_path.display().to_string()),
            target_physical_ms,
            target_logical,
            shard_plans.join(",")
        ))
    }

    fn pitr_restore_plan_shards(
        &self,
        db: &Neo4rDatabaseHandle,
        target_physical_ms: u64,
        target_logical: u32,
    ) -> Result<Vec<String>, String> {
        let target = neo4r_core::HybridTimestamp::new(target_physical_ms, target_logical);
        let committed = db.committed_indexes().map_err(|err| err.to_string())?;
        let mut shard_plans = Vec::new();
        for (shard_id, committed_index) in committed.iter().copied().enumerate() {
            let selected = db
                .log_entries_from(shard_id as u64, 1)
                .map_err(|err| err.to_string())?
                .into_iter()
                .filter(|entry| entry.index <= committed_index && entry.timestamp <= target)
                .collect::<Vec<_>>();
            let target_index = selected.last().map(|entry| entry.index).unwrap_or_default();
            shard_plans.push(format!(
                "{{\"shard_id\":{},\"committed_index\":{},\"target_index\":{},\"selected_entries\":{}}}",
                shard_id,
                committed_index,
                target_index,
                selected.len()
            ));
        }
        Ok(shard_plans)
    }
}
