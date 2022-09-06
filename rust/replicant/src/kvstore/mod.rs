pub mod memkvstore;

pub const NOT_FOUND: &str = "key not found";
pub const PUT_FAILED: &str = "put failed";

#[derive(PartialEq, Debug, Clone)]
pub enum Command {
    Get(String),
    Put(String, String),
    Del(String),
}

pub trait KVStore {
    fn get(&self, key: &str) -> Option<String>;
    fn put(&mut self, key: &str, value: &str) -> bool;
    fn del(&mut self, key: &str) -> bool;
}

impl Command {
    pub fn execute(&self, store: &mut impl KVStore) -> Result<Option<String>, &str> {
        match self {
            Command::Get(key) => match store.get(&key) {
                Some(value) => Ok(Some(value)),
                None => Err(NOT_FOUND),
            },
            Command::Put(key, value) => {
                if store.put(&key, &value) {
                    Ok(None)
                } else {
                    Err(PUT_FAILED)
                }
            }
            Command::Del(key) => {
                if store.del(&key) {
                    Ok(None)
                } else {
                    Err(NOT_FOUND)
                }
            }
        }
    }
}
