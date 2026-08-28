use neo4r_db::{DatabaseConfig, Neo4rDatabaseHandle};
use neo4r_storage::{KeyValueStore, RocksKvStore};
use std::collections::BTreeMap;
use std::fs;
use std::io::{self, ErrorKind};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

pub(super) const DEFAULT_DATABASE: &str = "default";
pub(super) const SYSTEM_DIR: &str = "system";
const CATALOG_DB_PREFIX: &[u8] = b"tenant/db/";

#[derive(Clone)]
pub(super) struct TenantDatabaseManager {
    default_db: Neo4rDatabaseHandle,
    root_dir: PathBuf,
    template: DatabaseConfig,
    databases: Arc<Mutex<BTreeMap<String, Neo4rDatabaseHandle>>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct TenantDatabaseRecord {
    pub(super) name: String,
    pub(super) disabled: bool,
}

impl TenantDatabaseManager {
    pub(super) fn open(
        default_db: Neo4rDatabaseHandle,
        template: DatabaseConfig,
    ) -> io::Result<Self> {
        let root_dir = template.data_dir.clone();
        let manager = Self {
            default_db,
            root_dir,
            template,
            databases: Arc::new(Mutex::new(BTreeMap::new())),
        };
        fs::create_dir_all(manager.databases_dir())?;
        for database in manager.catalog_records()? {
            if database.name != DEFAULT_DATABASE && !database.disabled {
                let _ = manager.database(&database.name)?;
            }
        }
        Ok(manager)
    }

    pub(super) fn database(&self, name: &str) -> io::Result<Neo4rDatabaseHandle> {
        validate_database_name(name).map_err(io::Error::other)?;
        if name == DEFAULT_DATABASE {
            return Ok(self.default_db.clone());
        }
        let mut databases = self
            .databases
            .lock()
            .map_err(|_| io::Error::other("tenant database lock poisoned"))?;
        if let Some(database) = databases.get(name) {
            return Ok(database.clone());
        }
        let Some(record) = self
            .catalog_records()?
            .into_iter()
            .find(|existing| existing.name == name)
        else {
            return Err(io::Error::new(
                ErrorKind::NotFound,
                format!("unknown database {name:?}"),
            ));
        };
        if record.disabled {
            return Err(io::Error::other(format!("database {name:?} is disabled")));
        }
        let database = self.open_named_database(name)?;
        databases.insert(name.to_string(), database.clone());
        Ok(database)
    }

    pub(super) fn create_database(&self, name: &str) -> io::Result<Neo4rDatabaseHandle> {
        validate_database_name(name).map_err(io::Error::other)?;
        if name == DEFAULT_DATABASE {
            return Ok(self.default_db.clone());
        }
        if let Some(database) = self
            .databases
            .lock()
            .map_err(|_| io::Error::other("tenant database lock poisoned"))?
            .get(name)
            .cloned()
        {
            return Ok(database);
        }
        let mut records = self.catalog_records()?;
        if let Some(record) = records.iter_mut().find(|record| record.name == name) {
            record.disabled = false;
            self.save_catalog_records(&records)?;
        } else {
            records.push(TenantDatabaseRecord {
                name: name.to_string(),
                disabled: false,
            });
            self.save_catalog_records(&records)?;
        }
        let database = self.open_named_database(name)?;
        self.databases
            .lock()
            .map_err(|_| io::Error::other("tenant database lock poisoned"))?
            .insert(name.to_string(), database.clone());
        Ok(database)
    }

    pub(super) fn list_database_records(&self) -> io::Result<Vec<TenantDatabaseRecord>> {
        self.catalog_records()
    }

    pub(super) fn disable_database(&self, name: &str) -> io::Result<()> {
        self.set_database_disabled(name, true)
    }

    pub(super) fn enable_database(&self, name: &str) -> io::Result<()> {
        self.set_database_disabled(name, false)
    }

    pub(super) fn delete_database(&self, name: &str) -> io::Result<()> {
        validate_database_name(name).map_err(io::Error::other)?;
        if name == DEFAULT_DATABASE {
            return Err(io::Error::other("default database cannot be deleted"));
        }
        let mut records = self.catalog_records()?;
        let Some(existing) = records.iter().find(|record| record.name == name) else {
            return Err(io::Error::new(
                ErrorKind::NotFound,
                format!("unknown database {name:?}"),
            ));
        };
        if !existing.disabled {
            return Err(io::Error::other(format!(
                "database {name:?} must be disabled before delete"
            )));
        }
        let before = records.len();
        records.retain(|record| record.name != name);
        if records.len() == before {
            return Err(io::Error::new(
                ErrorKind::NotFound,
                format!("unknown database {name:?}"),
            ));
        }
        self.save_catalog_records(&records)?;
        self.databases
            .lock()
            .map_err(|_| io::Error::other("tenant database lock poisoned"))?
            .remove(name);
        Ok(())
    }

    pub(super) fn databases_dir(&self) -> PathBuf {
        self.root_dir.join("databases")
    }

    pub(super) fn system_dir(&self) -> PathBuf {
        self.root_dir.join(SYSTEM_DIR)
    }

    fn open_named_database(&self, name: &str) -> io::Result<Neo4rDatabaseHandle> {
        let mut config = self.template.clone();
        config.data_dir = self.databases_dir().join(name);
        Neo4rDatabaseHandle::open(config).map_err(io::Error::other)
    }

    fn catalog_path(&self) -> PathBuf {
        self.system_dir().join("databases").join("catalog.txt")
    }

    fn legacy_catalog_path(&self) -> PathBuf {
        self.databases_dir().join("catalog.txt")
    }

    fn catalog_records(&self) -> io::Result<Vec<TenantDatabaseRecord>> {
        let mut records = self.load_rocks_catalog_records()?;
        if records.is_empty() {
            records = self.load_text_catalog_records()?;
            self.save_catalog_records(&records)?;
        }
        if !records.iter().any(|record| record.name == DEFAULT_DATABASE) {
            records.push(TenantDatabaseRecord {
                name: DEFAULT_DATABASE.to_string(),
                disabled: false,
            });
        }
        records.sort_by(|left, right| left.name.cmp(&right.name));
        records.dedup_by(|left, right| left.name == right.name);
        Ok(records)
    }

    fn load_text_catalog_records(&self) -> io::Result<Vec<TenantDatabaseRecord>> {
        let path = self.catalog_path();
        let text = match fs::read_to_string(&path) {
            Ok(text) => text,
            Err(err) if err.kind() == ErrorKind::NotFound => {
                if let Ok(text) = fs::read_to_string(self.legacy_catalog_path()) {
                    text
                } else {
                    return Ok(vec![TenantDatabaseRecord {
                        name: DEFAULT_DATABASE.to_string(),
                        disabled: false,
                    }]);
                }
            }
            Err(err) => return Err(err),
        };
        let mut records = Vec::new();
        for line in text.lines() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            validate_database_name(line).map_err(io::Error::other)?;
            if !records
                .iter()
                .any(|record: &TenantDatabaseRecord| record.name == line)
            {
                records.push(TenantDatabaseRecord {
                    name: line.to_string(),
                    disabled: false,
                });
            }
        }
        Ok(records)
    }

    fn load_rocks_catalog_records(&self) -> io::Result<Vec<TenantDatabaseRecord>> {
        if let Some(parent) = self.catalog_rocks_dir().parent() {
            fs::create_dir_all(parent)?;
        }
        let kv = RocksKvStore::open(self.catalog_rocks_dir()).map_err(io::Error::other)?;
        let records = kv
            .scan_prefix(CATALOG_DB_PREFIX)
            .map_err(io::Error::other)?;
        records
            .into_iter()
            .map(|(_, value)| decode_catalog_record(&String::from_utf8_lossy(&value)))
            .collect()
    }

    fn save_catalog_records(&self, records: &[TenantDatabaseRecord]) -> io::Result<()> {
        if let Some(parent) = self.catalog_rocks_dir().parent() {
            fs::create_dir_all(parent)?;
        }
        let mut kv = RocksKvStore::open(self.catalog_rocks_dir()).map_err(io::Error::other)?;
        for (key, _) in kv
            .scan_prefix(CATALOG_DB_PREFIX)
            .map_err(io::Error::other)?
        {
            kv.delete(&key).map_err(io::Error::other)?;
        }
        let mut records = records.to_vec();
        if !records.iter().any(|record| record.name == DEFAULT_DATABASE) {
            records.push(TenantDatabaseRecord {
                name: DEFAULT_DATABASE.to_string(),
                disabled: false,
            });
        }
        records.sort_by(|left, right| left.name.cmp(&right.name));
        records.dedup_by(|left, right| left.name == right.name);
        for record in records {
            validate_database_name(&record.name).map_err(io::Error::other)?;
            kv.put(
                &catalog_record_key(&record.name),
                encode_catalog_record(&record).as_bytes(),
            )
            .map_err(io::Error::other)?;
        }
        Ok(())
    }

    fn set_database_disabled(&self, name: &str, disabled: bool) -> io::Result<()> {
        validate_database_name(name).map_err(io::Error::other)?;
        if name == DEFAULT_DATABASE {
            return Err(io::Error::other(
                "default database lifecycle cannot be changed",
            ));
        }
        let mut records = self.catalog_records()?;
        let Some(record) = records.iter_mut().find(|record| record.name == name) else {
            return Err(io::Error::new(
                ErrorKind::NotFound,
                format!("unknown database {name:?}"),
            ));
        };
        record.disabled = disabled;
        self.save_catalog_records(&records)?;
        if disabled {
            self.databases
                .lock()
                .map_err(|_| io::Error::other("tenant database lock poisoned"))?
                .remove(name);
        }
        Ok(())
    }

    fn catalog_rocks_dir(&self) -> PathBuf {
        self.system_namespace_dir("catalog-rocksdb")
    }

    fn system_namespace_dir(&self, namespace: &str) -> PathBuf {
        self.system_dir().join(namespace)
    }
}

fn catalog_record_key(name: &str) -> Vec<u8> {
    let mut key = CATALOG_DB_PREFIX.to_vec();
    key.extend_from_slice(name.as_bytes());
    key
}

fn encode_catalog_record(record: &TenantDatabaseRecord) -> String {
    format!("{}\t{}\n", record.name, u8::from(record.disabled))
}

fn decode_catalog_record(input: &str) -> io::Result<TenantDatabaseRecord> {
    let parts = input.trim_end().split('\t').collect::<Vec<_>>();
    if parts.len() != 2 {
        return Err(io::Error::other("invalid tenant catalog record"));
    }
    validate_database_name(parts[0]).map_err(io::Error::other)?;
    Ok(TenantDatabaseRecord {
        name: parts[0].to_string(),
        disabled: parts[1] == "1",
    })
}

pub(super) fn validate_database_name(name: &str) -> Result<(), String> {
    if name.is_empty() || name.len() > 64 {
        return Err("database name must be 1..64 characters".to_string());
    }
    if name == "." || name == ".." || name == SYSTEM_DIR {
        return Err("database name cannot be a relative path segment".to_string());
    }
    if !name
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-')
    {
        return Err(
            "database name must contain only ASCII letters, digits, '_' or '-'".to_string(),
        );
    }
    Ok(())
}
