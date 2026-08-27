use crate::{StorageError, StorageResult};
use std::fs::{self, File, OpenOptions};
use std::io::{ErrorKind, Read, Write};
use std::path::{Path, PathBuf};

const MAGIC: &[u8] = b"N4RIC1\n";

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum IndexKind {
    NodeProperty,
    UniqueNodeProperty,
    Vector { dimensions: usize, metric: String },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IndexDefinition {
    pub name: String,
    pub label: String,
    pub property: String,
    pub kind: IndexKind,
}

impl IndexDefinition {
    pub fn node_property(
        name: impl Into<String>,
        label: impl Into<String>,
        property: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            label: label.into(),
            property: property.into(),
            kind: IndexKind::NodeProperty,
        }
    }

    pub fn vector(
        name: impl Into<String>,
        label: impl Into<String>,
        property: impl Into<String>,
        dimensions: usize,
        metric: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            label: label.into(),
            property: property.into(),
            kind: IndexKind::Vector {
                dimensions,
                metric: metric.into(),
            },
        }
    }

    pub fn unique_node_property(
        name: impl Into<String>,
        label: impl Into<String>,
        property: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            label: label.into(),
            property: property.into(),
            kind: IndexKind::UniqueNodeProperty,
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct IndexCatalog {
    pub version: u64,
    pub indexes: Vec<IndexDefinition>,
}

#[derive(Clone, Debug)]
pub struct IndexCatalogStore {
    path: PathBuf,
}

impl IndexCatalogStore {
    pub fn open(data_dir: impl AsRef<Path>) -> StorageResult<Self> {
        let index_dir = data_dir.as_ref().join("indexes");
        fs::create_dir_all(&index_dir)?;
        Ok(Self {
            path: index_dir.join("catalog.bin"),
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn save(&self, catalog: &IndexCatalog) -> StorageResult<()> {
        let tmp_path = self.path.with_extension("bin.tmp");
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp_path)?;

        file.write_all(MAGIC)?;
        write_u64(&mut file, catalog.version)?;
        write_u64(&mut file, catalog.indexes.len() as u64)?;
        for index in &catalog.indexes {
            write_string(&mut file, &index.name)?;
            write_string(&mut file, &index.label)?;
            write_string(&mut file, &index.property)?;
            match &index.kind {
                IndexKind::NodeProperty => write_u8(&mut file, 1)?,
                IndexKind::UniqueNodeProperty => write_u8(&mut file, 3)?,
                IndexKind::Vector { dimensions, metric } => {
                    write_u8(&mut file, 2)?;
                    write_u64(&mut file, *dimensions as u64)?;
                    write_string(&mut file, metric)?;
                }
            }
        }
        file.sync_all()?;
        drop(file);
        fs::rename(&tmp_path, &self.path)?;
        sync_parent_dir(&self.path)?;
        Ok(())
    }

    pub fn load(&self) -> StorageResult<Option<IndexCatalog>> {
        let mut file = match File::open(&self.path) {
            Ok(file) => file,
            Err(err) if err.kind() == ErrorKind::NotFound => return Ok(None),
            Err(err) => return Err(StorageError::Io(err)),
        };
        validate_header(&mut file)?;
        let version = read_u64(&mut file)?;
        let index_count = read_u64(&mut file)?;
        let mut indexes = Vec::with_capacity(index_count as usize);
        for _ in 0..index_count {
            let name = read_string(&mut file)?;
            let label = read_string(&mut file)?;
            let property = read_string(&mut file)?;
            let kind = match read_u8(&mut file)? {
                1 => IndexKind::NodeProperty,
                2 => {
                    let dimensions = read_u64(&mut file)? as usize;
                    let metric = read_string(&mut file)?;
                    IndexKind::Vector { dimensions, metric }
                }
                3 => IndexKind::UniqueNodeProperty,
                value => {
                    return Err(StorageError::CorruptStore(format!(
                        "unknown index kind {value}"
                    )))
                }
            };
            indexes.push(IndexDefinition {
                name,
                label,
                property,
                kind,
            });
        }
        ensure_eof(&mut file)?;
        Ok(Some(IndexCatalog { version, indexes }))
    }
}

fn validate_header(file: &mut File) -> StorageResult<()> {
    let mut header = [0; MAGIC.len()];
    file.read_exact(&mut header).map_err(|err| {
        if err.kind() == ErrorKind::UnexpectedEof {
            StorageError::CorruptStore("missing index catalog header".to_string())
        } else {
            StorageError::Io(err)
        }
    })?;
    if header == MAGIC {
        Ok(())
    } else {
        Err(StorageError::CorruptStore(
            "invalid index catalog header".to_string(),
        ))
    }
}

fn write_u8(file: &mut File, value: u8) -> StorageResult<()> {
    file.write_all(&[value])?;
    Ok(())
}

fn write_u64(file: &mut File, value: u64) -> StorageResult<()> {
    file.write_all(&value.to_be_bytes())?;
    Ok(())
}

fn write_string(file: &mut File, value: &str) -> StorageResult<()> {
    write_u64(file, value.len() as u64)?;
    file.write_all(value.as_bytes())?;
    Ok(())
}

fn read_u8(file: &mut File) -> StorageResult<u8> {
    let mut bytes = [0; 1];
    file.read_exact(&mut bytes)
        .map_err(map_truncated("index catalog u8"))?;
    Ok(bytes[0])
}

fn read_u64(file: &mut File) -> StorageResult<u64> {
    let mut bytes = [0; 8];
    file.read_exact(&mut bytes)
        .map_err(map_truncated("index catalog u64"))?;
    Ok(u64::from_be_bytes(bytes))
}

fn read_string(file: &mut File) -> StorageResult<String> {
    let len = read_u64(file)? as usize;
    let mut bytes = vec![0; len];
    file.read_exact(&mut bytes)
        .map_err(map_truncated("index catalog string"))?;
    String::from_utf8(bytes)
        .map_err(|_| StorageError::CorruptStore("index catalog string is not utf-8".to_string()))
}

fn map_truncated(field: &'static str) -> impl FnOnce(std::io::Error) -> StorageError + 'static {
    move |err| {
        if err.kind() == ErrorKind::UnexpectedEof {
            StorageError::CorruptStore(format!("truncated {field}"))
        } else {
            StorageError::Io(err)
        }
    }
}

fn ensure_eof(file: &mut File) -> StorageResult<()> {
    let mut trailing = [0; 1];
    match file.read(&mut trailing)? {
        0 => Ok(()),
        _ => Err(StorageError::CorruptStore(
            "trailing index catalog bytes".to_string(),
        )),
    }
}

fn sync_parent_dir(path: &Path) -> StorageResult<()> {
    if let Some(parent) = path.parent() {
        File::open(parent)?.sync_all()?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn missing_catalog_loads_as_none() {
        let dir = temp_dir("neo4r-missing-index-catalog");
        let store = IndexCatalogStore::open(&dir).unwrap();

        assert_eq!(store.load().unwrap(), None);

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn saves_and_loads_index_catalog() {
        let dir = temp_dir("neo4r-index-catalog");
        let store = IndexCatalogStore::open(&dir).unwrap();
        let catalog = IndexCatalog {
            version: 2,
            indexes: vec![
                IndexDefinition::node_property("person_name", "Person", "name"),
                IndexDefinition::unique_node_property("person_email_unique", "Person", "email"),
                IndexDefinition::vector("doc_embedding", "Document", "embedding", 384, "cosine"),
            ],
        };

        store.save(&catalog).unwrap();

        assert_eq!(store.load().unwrap(), Some(catalog));

        let _ = fs::remove_dir_all(dir);
    }

    fn temp_dir(prefix: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "{}-{}",
            prefix,
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ))
    }
}
