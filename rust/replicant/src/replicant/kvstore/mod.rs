pub mod memkvstore;

use thiserror::Error;

use super::multipaxos::rpc::{Command, CommandType};

#[derive(Debug, Error, PartialEq, Eq)]
pub enum KVStoreError {
    #[error("key not found")]
    NotFoundError,
    #[error("put failed")]
    PutFailedError,
}

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
    ) -> Result<String, KVStoreError> {
        match CommandType::from_i32(self.r#type).unwrap() {
            CommandType::Get => match store.get(&self.key) {
                Some(value) => Ok(value),
                None => Err(KVStoreError::NotFoundError),
            },
            CommandType::Put => {
                if store.put(&self.key, &self.value) {
                    Ok("".to_string())
                } else {
                    Err(KVStoreError::PutFailedError)
                }
            }
            CommandType::Del => {
                if store.del(&self.key) {
                    Ok("".to_string())
                } else {
                    Err(KVStoreError::NotFoundError)
                }
            }
        }
    }
}
