// The key-value store interface.
pub trait KVStore {
    fn new() -> Self;

    fn get(&self, key: &String) -> Option<&String>;

    fn put(&mut self, key: String, value: String)
           -> Result<(), &'static str>;

    fn del(&mut self, key: &String)
           -> Result<(), &'static str>;
}
