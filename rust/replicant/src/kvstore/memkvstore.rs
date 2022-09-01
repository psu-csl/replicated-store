use super::*;
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
        match self.map.get(key) {
            Some(value) => Some(value.clone()),
            None => None,
        }
    }

    fn put(&mut self, key: &str, value: &str) -> bool {
        self.map.insert(key.to_string(), value.to_string());
        true
    }

    fn del(&mut self, key: &str) -> bool {
        match self.map.remove(key) {
            Some(_) => true,
            None => false,
        }
    }
}

#[cfg(test)]
mod tests {
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
        let mut store = MemKVStore::new();

        let get_key1 = Command::Get(KEY1.to_string());
        let get_key2 = Command::Get(KEY2.to_string());
        let del_key1 = Command::Del(KEY1.to_string());
        let put_key1val1 = Command::Put(KEY1.to_string(), VAL1.to_string());
        let put_key2val2 = Command::Put(KEY2.to_string(), VAL2.to_string());
        let put_key1val2 = Command::Put(KEY1.to_string(), VAL2.to_string());

        assert_eq!(Err(NOT_FOUND), get_key1.execute(&mut store));

        assert_eq!(Err(NOT_FOUND), del_key1.execute(&mut store));

        assert_eq!(Ok(None), put_key1val1.execute(&mut store));
        assert_eq!(Ok(Some(VAL1.to_string())), get_key1.execute(&mut store));

        assert_eq!(Ok(None), put_key2val2.execute(&mut store));
        assert_eq!(Ok(Some(VAL2.to_string())), get_key2.execute(&mut store));

        assert_eq!(Ok(None), put_key1val2.execute(&mut store));
        assert_eq!(Ok(Some(VAL2.to_string())), get_key1.execute(&mut store));
        assert_eq!(Ok(Some(VAL2.to_string())), get_key2.execute(&mut store));

        assert_eq!(Ok(None), del_key1.execute(&mut store));
        assert_eq!(Err(NOT_FOUND), get_key1.execute(&mut store));
        assert_eq!(Ok(Some(VAL2.to_string())), get_key2.execute(&mut store));
    }
}
