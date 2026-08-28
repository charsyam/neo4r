use crate::StorageResult;
use std::collections::BTreeMap;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum KvWrite {
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct KvWriteBatch {
    operations: Vec<KvWrite>,
}

impl KvWriteBatch {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn put(&mut self, key: Vec<u8>, value: Vec<u8>) {
        self.operations.push(KvWrite::Put { key, value });
    }

    pub fn delete(&mut self, key: Vec<u8>) {
        self.operations.push(KvWrite::Delete { key });
    }

    pub fn is_empty(&self) -> bool {
        self.operations.is_empty()
    }

    pub fn operations(&self) -> &[KvWrite] {
        &self.operations
    }
}

pub trait KeyValueStore {
    fn get(&self, key: &[u8]) -> StorageResult<Option<Vec<u8>>>;
    fn put(&mut self, key: &[u8], value: &[u8]) -> StorageResult<()>;
    fn delete(&mut self, key: &[u8]) -> StorageResult<()>;
    fn write_batch(&mut self, batch: KvWriteBatch) -> StorageResult<()>;
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

    fn write_batch(&mut self, batch: KvWriteBatch) -> StorageResult<()> {
        let mut data = self.data.clone();
        for operation in batch.operations {
            match operation {
                KvWrite::Put { key, value } => {
                    data.insert(key, value);
                }
                KvWrite::Delete { key } => {
                    data.remove(&key);
                }
            }
        }
        self.data = data;
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
