// In-memory key-value store that implements the KVStore interface.

use std::collections::HashMap;
use crate::kvstore::traits::KVStore;

pub struct MemStore {
    map: HashMap<String, String>
}

impl KVStore for MemStore {
    fn new() -> MemStore {
        MemStore { map: HashMap::new() }
    }

    fn get(&self, key: &str) -> Result<&str, &str> {
        match self.map.get(key) {
            Some(val) => Ok(val),
            None => Err("not found")
        }
    }

    fn put(&mut self, key: String, value: String) -> Result<&str, &str> {
        self.map.insert(key, value);
        // Inserting into HashMap never fails. Inserting into a persistent
        // key-value store may fail for various reasons, such as full disk, in
        // which case we may need to return Err values.
        Ok("ok")
    }

    fn del(&mut self, key: &str) -> Result<&str, &str> {
        match self.map.remove(key) {
            Some(_) => Ok("ok"),
            None => Err("not found")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn put_get() {
        let mut store = MemStore::new();
        let (key, val) = (String::from("key"), String::from("val"));
        assert_eq!(store.put(key, val), Ok("ok"));
        let (key, val) = (String::from("key"), String::from("val"));
        assert_eq!(store.get(&key).unwrap(), &val);
    }

    #[test]
    fn get_nonexistent() {
        let store = MemStore::new();
        let key = String::from("key");
        assert_eq!(store.get(&key), Err("not found"));
    }

    #[test]
    fn put_del() {
        let mut store = MemStore::new();
        let (key, val) = (String::from("key"), String::from("val"));
        assert_eq!(store.put(key, val), Ok("ok"));
        let key = String::from("key");
        assert_eq!(store.del(&key), Ok("ok"));
        assert!(store.get(&key).is_err());
    }

    #[test]
    fn del_nonexistent() {
        let mut store: MemStore = KVStore::new();
        let key = String::from("key");
        assert!(store.del(&key).is_err());
    }
}
