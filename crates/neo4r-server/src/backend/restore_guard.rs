use crate::Neo4rDatabaseHandle;
use std::fs;
use std::path::{Path, PathBuf};

pub(crate) fn restore_maintenance_mode_path(db: &Neo4rDatabaseHandle) -> Result<PathBuf, String> {
    Ok(db
        .data_dir()
        .map_err(|err| err.to_string())?
        .join("system")
        .join("maintenance.mode"))
}

pub(crate) struct RestoreLock {
    path: PathBuf,
}

impl RestoreLock {
    pub(crate) fn acquire(target: &Path) -> Result<Self, String> {
        let path = target.join("system").join("restore.lock");
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|err| err.to_string())?;
        }
        std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .map_err(|err| format!("restore lock is already held or unavailable: {err}"))?;
        Ok(Self { path })
    }
}

impl Drop for RestoreLock {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}
