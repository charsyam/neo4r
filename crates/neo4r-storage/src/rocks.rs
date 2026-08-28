use crate::{KeyValueStore, KvWrite, KvWriteBatch, StorageError, StorageResult};
use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_uchar, c_void};
use std::os::unix::ffi::OsStrExt;
use std::path::Path;
use std::ptr;
use std::slice;

#[repr(C)]
struct rocksdb_t {
    _private: [u8; 0],
}

#[repr(C)]
struct rocksdb_options_t {
    _private: [u8; 0],
}

#[repr(C)]
struct rocksdb_readoptions_t {
    _private: [u8; 0],
}

#[repr(C)]
struct rocksdb_writeoptions_t {
    _private: [u8; 0],
}

#[repr(C)]
struct rocksdb_iterator_t {
    _private: [u8; 0],
}

#[repr(C)]
struct rocksdb_snapshot_t {
    _private: [u8; 0],
}

#[repr(C)]
struct rocksdb_writebatch_t {
    _private: [u8; 0],
}

#[link(name = "rocksdb")]
unsafe extern "C" {
    fn rocksdb_options_create() -> *mut rocksdb_options_t;
    fn rocksdb_options_destroy(options: *mut rocksdb_options_t);
    fn rocksdb_options_set_create_if_missing(options: *mut rocksdb_options_t, value: c_uchar);

    fn rocksdb_open(
        options: *const rocksdb_options_t,
        name: *const c_char,
        errptr: *mut *mut c_char,
    ) -> *mut rocksdb_t;
    fn rocksdb_close(db: *mut rocksdb_t);
    fn rocksdb_free(ptr: *mut c_void);

    fn rocksdb_readoptions_create() -> *mut rocksdb_readoptions_t;
    fn rocksdb_readoptions_destroy(options: *mut rocksdb_readoptions_t);
    fn rocksdb_readoptions_set_snapshot(
        options: *mut rocksdb_readoptions_t,
        snapshot: *const rocksdb_snapshot_t,
    );
    fn rocksdb_writeoptions_create() -> *mut rocksdb_writeoptions_t;
    fn rocksdb_writeoptions_destroy(options: *mut rocksdb_writeoptions_t);
    fn rocksdb_writeoptions_set_sync(options: *mut rocksdb_writeoptions_t, value: c_uchar);
    fn rocksdb_create_snapshot(db: *mut rocksdb_t) -> *const rocksdb_snapshot_t;
    fn rocksdb_release_snapshot(db: *mut rocksdb_t, snapshot: *const rocksdb_snapshot_t);

    fn rocksdb_put(
        db: *mut rocksdb_t,
        options: *const rocksdb_writeoptions_t,
        key: *const c_char,
        keylen: usize,
        value: *const c_char,
        valuelen: usize,
        errptr: *mut *mut c_char,
    );
    fn rocksdb_get(
        db: *mut rocksdb_t,
        options: *const rocksdb_readoptions_t,
        key: *const c_char,
        keylen: usize,
        vallen: *mut usize,
        errptr: *mut *mut c_char,
    ) -> *mut c_char;
    fn rocksdb_delete(
        db: *mut rocksdb_t,
        options: *const rocksdb_writeoptions_t,
        key: *const c_char,
        keylen: usize,
        errptr: *mut *mut c_char,
    );
    fn rocksdb_write(
        db: *mut rocksdb_t,
        options: *const rocksdb_writeoptions_t,
        batch: *mut rocksdb_writebatch_t,
        errptr: *mut *mut c_char,
    );
    fn rocksdb_writebatch_create() -> *mut rocksdb_writebatch_t;
    fn rocksdb_writebatch_destroy(batch: *mut rocksdb_writebatch_t);
    fn rocksdb_writebatch_put(
        batch: *mut rocksdb_writebatch_t,
        key: *const c_char,
        keylen: usize,
        value: *const c_char,
        valuelen: usize,
    );
    fn rocksdb_writebatch_delete(
        batch: *mut rocksdb_writebatch_t,
        key: *const c_char,
        keylen: usize,
    );

    fn rocksdb_create_iterator(
        db: *mut rocksdb_t,
        options: *const rocksdb_readoptions_t,
    ) -> *mut rocksdb_iterator_t;
    fn rocksdb_iter_destroy(iterator: *mut rocksdb_iterator_t);
    fn rocksdb_iter_seek(iterator: *mut rocksdb_iterator_t, key: *const c_char, keylen: usize);
    fn rocksdb_iter_valid(iterator: *const rocksdb_iterator_t) -> c_uchar;
    fn rocksdb_iter_next(iterator: *mut rocksdb_iterator_t);
    fn rocksdb_iter_key(iterator: *const rocksdb_iterator_t, klen: *mut usize) -> *const c_char;
    fn rocksdb_iter_value(iterator: *const rocksdb_iterator_t, vlen: *mut usize) -> *const c_char;
    fn rocksdb_iter_get_error(iterator: *const rocksdb_iterator_t, errptr: *mut *mut c_char);
}

pub struct RocksKvStore {
    db: *mut rocksdb_t,
    read_options: *mut rocksdb_readoptions_t,
    write_options: *mut rocksdb_writeoptions_t,
    options: RocksKvOptions,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct RocksKvOptions {
    pub sync_writes: bool,
}

// RocksDB's C handle can be moved across threads. neo4r currently serializes
// access to each database handle at a higher layer.
unsafe impl Send for RocksKvStore {}

impl RocksKvStore {
    pub fn open(path: impl AsRef<Path>) -> StorageResult<Self> {
        Self::open_with_options(path, RocksKvOptions::default())
    }

    pub fn open_with_options(
        path: impl AsRef<Path>,
        options: RocksKvOptions,
    ) -> StorageResult<Self> {
        let path = CString::new(path.as_ref().as_os_str().as_bytes()).map_err(|_| {
            StorageError::CorruptStore("rocksdb path contains nul byte".to_string())
        })?;

        unsafe {
            let db_options = rocksdb_options_create();
            if db_options.is_null() {
                return Err(StorageError::CorruptStore(
                    "failed to create rocksdb options".to_string(),
                ));
            }
            rocksdb_options_set_create_if_missing(db_options, 1);

            let mut err = ptr::null_mut();
            let db = rocksdb_open(db_options, path.as_ptr(), &mut err);
            rocksdb_options_destroy(db_options);
            check_error(err, "rocksdb open")?;
            if db.is_null() {
                return Err(StorageError::CorruptStore(
                    "rocksdb open returned null".to_string(),
                ));
            }

            let read_options = rocksdb_readoptions_create();
            let write_options = rocksdb_writeoptions_create();
            if read_options.is_null() || write_options.is_null() {
                if !read_options.is_null() {
                    rocksdb_readoptions_destroy(read_options);
                }
                if !write_options.is_null() {
                    rocksdb_writeoptions_destroy(write_options);
                }
                rocksdb_close(db);
                return Err(StorageError::CorruptStore(
                    "failed to create rocksdb read/write options".to_string(),
                ));
            }
            rocksdb_writeoptions_set_sync(write_options, u8::from(options.sync_writes));

            Ok(Self {
                db,
                read_options,
                write_options,
                options,
            })
        }
    }

    pub fn options(&self) -> RocksKvOptions {
        self.options
    }

    pub fn snapshot(&self) -> StorageResult<RocksKvSnapshot> {
        unsafe {
            let snapshot = rocksdb_create_snapshot(self.db);
            if snapshot.is_null() {
                return Err(StorageError::CorruptStore(
                    "failed to create rocksdb snapshot".to_string(),
                ));
            }

            let read_options = rocksdb_readoptions_create();
            if read_options.is_null() {
                rocksdb_release_snapshot(self.db, snapshot);
                return Err(StorageError::CorruptStore(
                    "failed to create rocksdb snapshot read options".to_string(),
                ));
            }
            rocksdb_readoptions_set_snapshot(read_options, snapshot);
            Ok(RocksKvSnapshot {
                db: self.db,
                snapshot,
                read_options,
            })
        }
    }
}

pub struct RocksKvSnapshot {
    db: *mut rocksdb_t,
    snapshot: *const rocksdb_snapshot_t,
    read_options: *mut rocksdb_readoptions_t,
}

// RocksDB snapshots are immutable read views. neo4r keeps each snapshot's read
// options owned by the snapshot handle and cursor fetches are serialized above.
unsafe impl Send for RocksKvSnapshot {}
unsafe impl Sync for RocksKvSnapshot {}

impl Drop for RocksKvSnapshot {
    fn drop(&mut self) {
        unsafe {
            rocksdb_readoptions_destroy(self.read_options);
            rocksdb_release_snapshot(self.db, self.snapshot);
        }
    }
}

impl KeyValueStore for RocksKvSnapshot {
    fn get(&self, key: &[u8]) -> StorageResult<Option<Vec<u8>>> {
        rocks_get(self.db, self.read_options, key)
    }

    fn put(&mut self, _key: &[u8], _value: &[u8]) -> StorageResult<()> {
        Err(StorageError::CorruptStore(
            "rocksdb snapshot is read-only".to_string(),
        ))
    }

    fn delete(&mut self, _key: &[u8]) -> StorageResult<()> {
        Err(StorageError::CorruptStore(
            "rocksdb snapshot is read-only".to_string(),
        ))
    }

    fn write_batch(&mut self, _batch: KvWriteBatch) -> StorageResult<()> {
        Err(StorageError::CorruptStore(
            "rocksdb snapshot is read-only".to_string(),
        ))
    }

    fn scan_prefix(&self, prefix: &[u8]) -> StorageResult<Vec<(Vec<u8>, Vec<u8>)>> {
        rocks_scan_prefix(self.db, self.read_options, prefix)
    }
}

impl Drop for RocksKvStore {
    fn drop(&mut self) {
        unsafe {
            rocksdb_readoptions_destroy(self.read_options);
            rocksdb_writeoptions_destroy(self.write_options);
            rocksdb_close(self.db);
        }
    }
}

impl KeyValueStore for RocksKvStore {
    fn get(&self, key: &[u8]) -> StorageResult<Option<Vec<u8>>> {
        rocks_get(self.db, self.read_options, key)
    }

    fn put(&mut self, key: &[u8], value: &[u8]) -> StorageResult<()> {
        unsafe {
            let mut err = ptr::null_mut();
            rocksdb_put(
                self.db,
                self.write_options,
                key.as_ptr().cast::<c_char>(),
                key.len(),
                value.as_ptr().cast::<c_char>(),
                value.len(),
                &mut err,
            );
            check_error(err, "rocksdb put")
        }
    }

    fn delete(&mut self, key: &[u8]) -> StorageResult<()> {
        unsafe {
            let mut err = ptr::null_mut();
            rocksdb_delete(
                self.db,
                self.write_options,
                key.as_ptr().cast::<c_char>(),
                key.len(),
                &mut err,
            );
            check_error(err, "rocksdb delete")
        }
    }

    fn write_batch(&mut self, batch: KvWriteBatch) -> StorageResult<()> {
        if batch.is_empty() {
            return Ok(());
        }
        unsafe {
            let rocks_batch = rocksdb_writebatch_create();
            if rocks_batch.is_null() {
                return Err(StorageError::CorruptStore(
                    "failed to create rocksdb write batch".to_string(),
                ));
            }

            for operation in batch.operations() {
                match operation {
                    KvWrite::Put { key, value } => rocksdb_writebatch_put(
                        rocks_batch,
                        key.as_ptr().cast::<c_char>(),
                        key.len(),
                        value.as_ptr().cast::<c_char>(),
                        value.len(),
                    ),
                    KvWrite::Delete { key } => rocksdb_writebatch_delete(
                        rocks_batch,
                        key.as_ptr().cast::<c_char>(),
                        key.len(),
                    ),
                }
            }

            let mut err = ptr::null_mut();
            rocksdb_write(self.db, self.write_options, rocks_batch, &mut err);
            rocksdb_writebatch_destroy(rocks_batch);
            check_error(err, "rocksdb write batch")
        }
    }

    fn scan_prefix(&self, prefix: &[u8]) -> StorageResult<Vec<(Vec<u8>, Vec<u8>)>> {
        rocks_scan_prefix(self.db, self.read_options, prefix)
    }
}

fn rocks_get(
    db: *mut rocksdb_t,
    read_options: *mut rocksdb_readoptions_t,
    key: &[u8],
) -> StorageResult<Option<Vec<u8>>> {
    unsafe {
        let mut err = ptr::null_mut();
        let mut value_len = 0;
        let value = rocksdb_get(
            db,
            read_options,
            key.as_ptr().cast::<c_char>(),
            key.len(),
            &mut value_len,
            &mut err,
        );
        check_error(err, "rocksdb get")?;
        if value.is_null() {
            return Ok(None);
        }
        let bytes = slice::from_raw_parts(value.cast::<u8>(), value_len).to_vec();
        rocksdb_free(value.cast::<c_void>());
        Ok(Some(bytes))
    }
}

fn rocks_scan_prefix(
    db: *mut rocksdb_t,
    read_options: *mut rocksdb_readoptions_t,
    prefix: &[u8],
) -> StorageResult<Vec<(Vec<u8>, Vec<u8>)>> {
    unsafe {
        let iterator = rocksdb_create_iterator(db, read_options);
        if iterator.is_null() {
            return Err(StorageError::CorruptStore(
                "failed to create rocksdb iterator".to_string(),
            ));
        }

        rocksdb_iter_seek(iterator, prefix.as_ptr().cast::<c_char>(), prefix.len());
        let mut rows = Vec::new();
        while rocksdb_iter_valid(iterator) != 0 {
            let mut key_len = 0;
            let key_ptr = rocksdb_iter_key(iterator, &mut key_len);
            let key = slice::from_raw_parts(key_ptr.cast::<u8>(), key_len);
            if !key.starts_with(prefix) {
                break;
            }

            let mut value_len = 0;
            let value_ptr = rocksdb_iter_value(iterator, &mut value_len);
            let value = slice::from_raw_parts(value_ptr.cast::<u8>(), value_len);
            rows.push((key.to_vec(), value.to_vec()));

            rocksdb_iter_next(iterator);
        }

        let mut err = ptr::null_mut();
        rocksdb_iter_get_error(iterator, &mut err);
        rocksdb_iter_destroy(iterator);
        check_error(err, "rocksdb iterator")?;

        Ok(rows)
    }
}

fn check_error(err: *mut c_char, context: &str) -> StorageResult<()> {
    if err.is_null() {
        return Ok(());
    }

    unsafe {
        let message = CStr::from_ptr(err).to_string_lossy().into_owned();
        rocksdb_free(err.cast::<c_void>());
        Err(StorageError::CorruptStore(format!("{context}: {message}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::KvGraphStore;
    use neo4r_core::{Command, Properties, Value};
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn persists_graph_indexes_in_rocksdb() {
        let dir = temp_dir("neo4r-rocksdb");

        {
            let rocks = RocksKvStore::open(&dir).unwrap();
            let mut store = KvGraphStore::new(rocks);
            store
                .apply(&Command::CreateNode {
                    id: 1,
                    labels: vec!["Person".to_string()],
                    properties: properties(&[("name", Value::String("Alice".to_string()))]),
                })
                .unwrap();
            store
                .apply(&Command::CreateNode {
                    id: 2,
                    labels: vec!["Person".to_string()],
                    properties: properties(&[("name", Value::String("Bob".to_string()))]),
                })
                .unwrap();
            store
                .apply(&Command::CreateRelationship {
                    id: 10,
                    from: 1,
                    to: 2,
                    rel_type: "KNOWS".to_string(),
                    properties: Properties::new(),
                })
                .unwrap();
        }

        {
            let rocks = RocksKvStore::open(&dir).unwrap();
            let store = KvGraphStore::new(rocks);

            assert_eq!(
                store
                    .node_ids_by_label_property(
                        "Person",
                        "name",
                        &Value::String("Alice".to_string())
                    )
                    .unwrap(),
                vec![1]
            );
            assert_eq!(store.outgoing_by_type(1, "KNOWS").unwrap()[0].to, 2);
        }

        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn opens_rocksdb_with_sync_write_durability_option() {
        let dir = temp_dir("neo4r-rocksdb-sync-writes");
        let rocks =
            RocksKvStore::open_with_options(&dir, RocksKvOptions { sync_writes: true }).unwrap();

        assert_eq!(rocks.options(), RocksKvOptions { sync_writes: true });

        let _ = std::fs::remove_dir_all(dir);
    }

    fn properties(entries: &[(&str, Value)]) -> Properties {
        entries
            .iter()
            .map(|(key, value)| ((*key).to_string(), value.clone()))
            .collect()
    }

    fn temp_dir(prefix: &str) -> std::path::PathBuf {
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
