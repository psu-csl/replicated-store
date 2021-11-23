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

    fn get(&self, key: &String) -> Option<&String> {
        self.map.get(key)
    }

    fn put(&mut self, key: String, value: String)
           -> Result<(), &'static str> {
        self.map.insert(key, value);
        // We may need to return non-Ok values for a persistent KVStore.
        Ok(())
    }

    fn del(&mut self, key: &String)
           -> Result<(), &'static str> {
        match self.map.remove(key) {
            Some(_) => Ok(()),
            None => Err("no such value")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn put_get() {
        let mut store: MemStore = KVStore::new();
        let (key, val) = (String::from("key"), String::from("val"));
        assert_eq!(store.put(key, val), Ok(()));
        let (key, val) = (String::from("key"), String::from("val"));
        assert_eq!(store.get(&key).unwrap(), &val);
    }

    #[test]
    fn get_nonexistent() {
        let store: MemStore = KVStore::new();
        let key = String::from("key");
        assert!(store.get(&key).is_none());
    }

    #[test]
    fn put_del() {
        let mut store: MemStore = KVStore::new();
        let (key, val) = (String::from("key"), String::from("val"));
        assert_eq!(store.put(key, val), Ok(()));
        let key = String::from("key");
        assert_eq!(store.del(&key), Ok(()));
        assert!(store.get(&key).is_none());
    }

    #[test]
    fn del_nonexistent() {
        let mut store: MemStore = KVStore::new();
        let key = String::from("key");
        assert!(store.del(&key).is_err());
    }
}
