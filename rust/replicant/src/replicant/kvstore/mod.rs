pub mod memkvstore;

pub const NOT_FOUND: &str = "key not found";
pub const PUT_FAILED: &str = "put failed";

use super::multipaxos::rpc::{Command, CommandType};

pub trait KVStore {
    fn get(&self, key: &str) -> Option<String>;
    fn put(&mut self, key: &str, value: &str) -> bool;
    fn del(&mut self, key: &str) -> bool;
}

impl Command {
    pub fn get(key: &str) -> Self {
        Self {
            r#type: CommandType::Get as i32,
            key: key.to_string(),
            value: String::from(""),
        }
    }

    pub fn put(key: &str, value: &str) -> Self {
        Self {
            r#type: CommandType::Put as i32,
            key: key.to_string(),
            value: value.to_string(),
        }
    }

    pub fn del(key: &str) -> Self {
        Self {
            r#type: CommandType::Del as i32,
            key: key.to_string(),
            value: String::from(""),
        }
    }

    pub fn execute(
        &self,
        store: &mut Box<dyn KVStore + Sync + Send>,
    ) -> Result<Option<String>, &'static str> {
        match CommandType::from_i32(self.r#type).unwrap() {
            CommandType::Get => match store.get(&self.key) {
                Some(value) => Ok(Some(value)),
                None => Err(NOT_FOUND),
            },
            CommandType::Put => {
                if store.put(&self.key, &self.value) {
                    Ok(None)
                } else {
                    Err(PUT_FAILED)
                }
            }
            CommandType::Del => {
                if store.del(&self.key) {
                    Ok(None)
                } else {
                    Err(NOT_FOUND)
                }
            }
        }
    }
}
