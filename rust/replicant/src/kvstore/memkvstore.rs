use super::*;
use std::collections::HashMap;

pub struct MemKVStore {
    map: HashMap<String, String>,
}

impl KVStore for MemKVStore {
    fn new() -> MemKVStore {
        MemKVStore {
            map: HashMap::new(),
        }
    }

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

    fn execute(&mut self, command: &Command) -> Result<Option<String>, &str> {
        match command {
            Command::Get(key) => match self.get(key) {
                Some(value) => Ok(Some(value)),
                None => Err(NOT_FOUND),
            },
            Command::Put(key, value) => {
                if self.put(key, value) {
                    Ok(None)
                } else {
                    Err(PUT_FAILED)
                }
            }
            Command::Del(key) => {
                if self.del(key) {
                    Ok(None)
                } else {
                    Err(NOT_FOUND)
                }
            }
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

        assert_eq!(Err(NOT_FOUND), store.execute(&get_key1));

        assert_eq!(Err(NOT_FOUND), store.execute(&del_key1));

        assert_eq!(Ok(None), store.execute(&put_key1val1));
        assert_eq!(Ok(Some(VAL1.to_string())), store.execute(&get_key1));

        assert_eq!(Ok(None), store.execute(&put_key2val2));
        assert_eq!(Ok(Some(VAL2.to_string())), store.execute(&get_key2));

        assert_eq!(Ok(None), store.execute(&put_key1val2));
        assert_eq!(Ok(Some(VAL2.to_string())), store.execute(&get_key1));
        assert_eq!(Ok(Some(VAL2.to_string())), store.execute(&get_key2));

        assert_eq!(Ok(None), store.execute(&del_key1));
        assert_eq!(Err(NOT_FOUND), store.execute(&get_key1));
        assert_eq!(Ok(Some(VAL2.to_string())), store.execute(&get_key2));
    }
}
