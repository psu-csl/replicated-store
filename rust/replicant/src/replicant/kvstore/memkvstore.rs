use super::KVStore;
use std::collections::HashMap;

pub struct MemKVStore {
    map: HashMap<String, String>,
}

impl MemKVStore {
    pub fn new() -> Self {
        MemKVStore {
            map: HashMap::new(),
        }
    }
}

impl KVStore for MemKVStore {
    fn get(&self, key: &str) -> Option<String> {
        self.map.get(key).cloned()
    }

    fn put(&mut self, key: &str, value: &str) -> bool {
        self.map.insert(key.to_string(), value.to_string());
        true
    }

    fn del(&mut self, key: &str) -> bool {
        self.map.remove(key).is_some()
    }
}

#[cfg(test)]
mod tests {
    use crate::replicant::{kvstore::KVStoreError, multipaxos::msg::Command};

    use super::*;

    const KEY1: &str = "foo";
    const VAL1: &str = "bar";

    const KEY2: &str = "baz";
    const VAL2: &str = "quux";

    #[test]
    fn get_put_del() {
        let mut store = MemKVStore::new();

        assert_eq!(None, store.get(KEY1));

        assert_eq!(false, store.del(KEY1));

        assert_eq!(true, store.put(KEY1, VAL1));
        assert_eq!(Some(VAL1.to_string()), store.get(KEY1));

        assert_eq!(true, store.put(KEY2, VAL2));
        assert_eq!(Some(VAL2.to_string()), store.get(KEY2));

        assert_eq!(true, store.put(KEY1, VAL2));
        assert_eq!(Some(VAL2.to_string()), store.get(KEY1));
        assert_eq!(Some(VAL2.to_string()), store.get(KEY2));

        assert_eq!(true, store.del(KEY1));
        assert_eq!(None, store.get(KEY1));
        assert_eq!(Some(VAL2.to_string()), store.get(KEY2));

        assert_eq!(true, store.del(KEY2));
        assert_eq!(None, store.get(KEY1));
        assert_eq!(None, store.get(KEY2));
    }

    #[test]
    fn execute() {
        let mut store: Box<dyn KVStore + Sync + Send> =
            Box::new(MemKVStore::new());

        let get_key1 = Command::get(KEY1);
        let get_key2 = Command::get(KEY2);
        let del_key1 = Command::del(KEY1);
        let put_key1val1 = Command::put(KEY1, VAL1);
        let put_key2val2 = Command::put(KEY2, VAL2);
        let put_key1val2 = Command::put(KEY1, VAL2);

        assert_eq!(
            Err(KVStoreError::NotFoundError),
            get_key1.execute(&mut store)
        );

        assert_eq!(
            Err(KVStoreError::NotFoundError),
            del_key1.execute(&mut store)
        );

        let empty = Ok("".to_string());

        assert_eq!(empty, put_key1val1.execute(&mut store));
        assert_eq!(Ok(VAL1.to_string()), get_key1.execute(&mut store));

        assert_eq!(empty, put_key2val2.execute(&mut store));
        assert_eq!(Ok(VAL2.to_string()), get_key2.execute(&mut store));

        assert_eq!(empty, put_key1val2.execute(&mut store));
        assert_eq!(Ok(VAL2.to_string()), get_key1.execute(&mut store));
        assert_eq!(Ok(VAL2.to_string()), get_key2.execute(&mut store));

        assert_eq!(empty, del_key1.execute(&mut store));
        assert_eq!(
            Err(KVStoreError::NotFoundError),
            get_key1.execute(&mut store)
        );
        assert_eq!(Ok(VAL2.to_string()), get_key2.execute(&mut store));
    }
}
