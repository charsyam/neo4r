use crate::StorageResult;
use std::collections::BTreeMap;

pub trait KeyValueStore {
    fn get(&self, key: &[u8]) -> StorageResult<Option<Vec<u8>>>;
    fn put(&mut self, key: &[u8], value: &[u8]) -> StorageResult<()>;
    fn delete(&mut self, key: &[u8]) -> StorageResult<()>;
    fn scan_prefix(&self, prefix: &[u8]) -> StorageResult<Vec<(Vec<u8>, Vec<u8>)>>;
}

#[derive(Clone, Debug, Default)]
pub struct MemoryKvStore {
    data: BTreeMap<Vec<u8>, Vec<u8>>,
}

impl MemoryKvStore {
    pub fn new() -> Self {
        Self::default()
    }
}

impl KeyValueStore for MemoryKvStore {
    fn get(&self, key: &[u8]) -> StorageResult<Option<Vec<u8>>> {
        Ok(self.data.get(key).cloned())
    }

    fn put(&mut self, key: &[u8], value: &[u8]) -> StorageResult<()> {
        self.data.insert(key.to_vec(), value.to_vec());
        Ok(())
    }

    fn delete(&mut self, key: &[u8]) -> StorageResult<()> {
        self.data.remove(key);
        Ok(())
    }

    fn scan_prefix(&self, prefix: &[u8]) -> StorageResult<Vec<(Vec<u8>, Vec<u8>)>> {
        Ok(self
            .data
            .range(prefix.to_vec()..)
            .take_while(|(key, _)| key.starts_with(prefix))
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect())
    }
}
