pub mod memkvstore;

pub const NOT_FOUND: &str = "key not found";
pub const PUT_FAILED: &str = "put failed";

pub enum Command {
    Get(String),
    Put(String, String),
    Del(String),
}

pub trait KVStore {
    fn new() -> Self
    where
        Self: Sized;

    fn get(&self, key: &str) -> Option<String>;

    fn put(&mut self, key: &str, value: &str) -> bool;

    fn del(&mut self, key: &str) -> bool;

    fn execute(&mut self, command: &Command) -> Result<Option<String>, &str>;
}
