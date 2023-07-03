use super::KVStore;
use rocksdb::{DB, Options};

pub struct RocksDBKVStore {
    db: DB
}

impl RocksDBKVStore {
    pub fn new(path: &str) -> Self {
        RocksDBKVStore{
            db: DB::open_default(path).unwrap(),
        }
    }
}

impl KVStore for RocksDBKVStore {
    fn get(&self, key: &str) -> Option<String> {
        match self.db.get(key.as_bytes()) {
            Ok(Some(value)) => Some(String::from_utf8(value).unwrap()),
            Ok(None) => None,
            Err(_) => None,
        }
    }

    fn put(&mut self, key: &str, value: &str) -> bool {
        match self.db.put(key.as_bytes(), value.as_bytes()) {
            Ok(_) => true,
            Err(_) => false,
        }
    }

    fn del(&mut self, key: &str) -> bool {
        match self.db.delete(key.as_bytes()) {
            Ok(_) => true,
            Err(_) => false,
        }
    }
}
